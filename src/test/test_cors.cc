#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
extern "C"{
#include <curl/curl.h>
}
#include "common/ceph_crypto.h"
#include <map>
#include <list>
#define S3_BUCKET_NAME "s3testgw.fcgi"
#define SWIFT_BUCKET_NAME "swift3testgw.fcgi"
#define BUCKET_URL \
  ((g_test->get_key_type() == KEY_TYPE_S3)?(string("/" S3_BUCKET_NAME)):(string("/swift/v1/" SWIFT_BUCKET_NAME)))
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
#include "common/code_environment.h"
#include "common/ceph_argparse.h"
#include "common/Finisher.h"
#include "global/global_init.h"
#include "rgw/rgw_cors.h"
#include "rgw/rgw_cors_s3.h"

using namespace std;

#define CURL_VERBOSE 0
#define HTTP_RESPONSE_STR "RespCode"
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20

extern "C" int ceph_armor(char *dst, const char *dst_end, 
                          const char *src, const char *end);
enum key_type {
  KEY_TYPE_UNDEFINED = 0,
  KEY_TYPE_SWIFT,
  KEY_TYPE_S3
};

static void print_usage(char *exec){
  cout << "Usage: " << exec << " <Options>\n";
  cout << "Options:\n"
          "-g <gw-ip> - The ip address of the gateway\n"
          "-p <gw-port> - The port number of the gateway\n"
          "-k <SWIFT|S3> - The key type, either SWIFT or S3\n"
          "-s3 <AWSAccessKeyId:SecretAccessKeyID> - Only, if the key type is S3, gives S3 credentials\n"
          "-swift <Auth-Token> - Only if the key type is SWIFT, and gives the SWIFT credentials\n";
}
class test_cors_helper {
  private:
    string host;
    string port;
    string creds;
    CURL *curl_inst;
    map<string, string> response;
    list<string> extra_hdrs;
    string *resp_data;
    unsigned resp_code;
    key_type kt;
  public:
    test_cors_helper() : curl_inst(NULL), resp_data(NULL), resp_code(0), kt(KEY_TYPE_UNDEFINED){
      curl_global_init(CURL_GLOBAL_ALL);
    }
    ~test_cors_helper(){
      curl_global_cleanup();
    }
    int send_request(string method, string uri, 
                     size_t (*function)(void *,size_t,size_t,void *) = 0,
                     void *ud = 0, size_t length = 0);
    int extract_input(unsigned argc, char *argv[]);
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
      /*cout << resp_data->c_str() << "\n";*/
    }
    const string *get_response_data(){return resp_data;}
    unsigned get_resp_code(){return resp_code;}
    key_type get_key_type(){return kt;}
};

int test_cors_helper::extract_input(unsigned argc, char *argv[]){
#define ERR_CHECK_NEXT_PARAM(o) \
  if((loop + 1) >= argc)return -1; \
  else o = argv[loop+1];

  for(unsigned loop = 1;loop < argc; loop += 2){
    if(strcmp(argv[loop], "-g") == 0){
      ERR_CHECK_NEXT_PARAM(host);
    }else if(strcmp(argv[loop], "-k") == 0){
      string type;
      ERR_CHECK_NEXT_PARAM(type);
      if(type.compare("S3") == 0)kt = KEY_TYPE_S3;
      else if(type.compare("SWIFT") == 0)kt = KEY_TYPE_SWIFT;
    }else if(strcmp(argv[loop],"-s3") == 0){
      ERR_CHECK_NEXT_PARAM(creds);
    }else if(strcmp(argv[loop],"-swift") == 0){
      ERR_CHECK_NEXT_PARAM(creds);
    }else if(strcmp(argv[loop],"-p") == 0){
      ERR_CHECK_NEXT_PARAM(port);
    }else return -1;
  }
  if(host.length() <= 0 ||
     creds.length() <= 0)
    return -1;
  return 0;
}

void test_cors_helper::set_response(char *r){
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
  test_cors_helper *h = static_cast<test_cors_helper *>(ud);
  h->set_response((char *)ptr);
  return size*nmemb;
}

size_t write_data(void *ptr, size_t size, size_t nmemb, void *ud){
  test_cors_helper *h = static_cast<test_cors_helper *>(ud);
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
  buf_to_hex((unsigned char *)dest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, hex_str);
}

static int get_s3_auth(string method, string creds, string date, string res, string& out){
  string aid, secret, auth_hdr;
  size_t off = creds.find(":");
  out = "";
  if(off != string::npos){
    aid.assign(creds, 0, off);
    secret.assign(creds, off + 1, string::npos);

    /*sprintf(auth_hdr, "%s\n\n\n%s\n%s", req_type, date, res);*/
    char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
    char b64[65]; /* 64 is really enough */
    auth_hdr.append(method + string("\n\n\n") + date + string("\n") + res);
    calc_hmac_sha1(secret.c_str(), secret.length(), auth_hdr.c_str(), auth_hdr.length(), hmac_sha1);
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

int test_cors_helper::send_request(string method, string res, 
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
    curl_easy_setopt(curl_inst, CURLOPT_HEADERFUNCTION, write_header);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEHEADER, (void *)this);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEFUNCTION, write_data);
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
    if(kt == KEY_TYPE_S3){
      string s3auth;
      if(get_s3_auth(method, creds, date, res, s3auth) < 0)return -1;
      auth.append(string("Authorization: AWS ") + s3auth);
    } else if(kt == KEY_TYPE_SWIFT){
      auth.append(string("X-Auth-Token: ") + creds);
    } else {
      cout << "Unknown state (" << kt << ")\n";
      return -1;
    }

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

test_cors_helper *g_test;
Finisher *finisher;

static int create_bucket(void){
  if(g_test->get_key_type() == KEY_TYPE_S3){
    g_test->send_request(string("PUT"), string("/" S3_BUCKET_NAME));
    if(g_test->get_resp_code() != 200U){
      cout << "Error creating bucket, http code " << g_test->get_resp_code();
      return -1;
    }
  }else if(g_test->get_key_type() == KEY_TYPE_SWIFT){
    g_test->send_request(string("PUT"), string("/swift/v1/" SWIFT_BUCKET_NAME));
    if(g_test->get_resp_code() != 201U){
      cout << "Error creating bucket, http code " << g_test->get_resp_code();
      return -1;
    }
  }else return -1;
  return 0;
}

static int delete_bucket(void){
  if(g_test->get_key_type() == KEY_TYPE_S3){
    g_test->send_request(string("DELETE"), string("/" S3_BUCKET_NAME));
    if(g_test->get_resp_code() != 204U){
      cout << "Error deleting bucket, http code " << g_test->get_resp_code();
      return -1;
    }
  }else if(g_test->get_key_type() == KEY_TYPE_SWIFT){
    g_test->send_request(string("DELETE"), string("/swift/v1/" SWIFT_BUCKET_NAME));
    if(g_test->get_resp_code() != 204U){
      cout << "Error deleting bucket, http code " << g_test->get_resp_code();
      return -1;
    }
  }else return -1;
  return 0;
}

RGWCORSRule *xml_to_cors_rule(string s){
  RGWCORSConfiguration_S3 *cors_config;
  RGWCORSXMLParser_S3 parser(g_ceph_context);
  const string *data = g_test->get_response_data();
  if (!parser.init()) {
    return NULL;
  }
  if (!parser.parse(data->c_str(), data->length(), 1)) {
    return NULL;
  }
  cors_config = (RGWCORSConfiguration_S3 *)parser.find_first("CORSConfiguration");
  if (!cors_config) {
    return NULL;
  }
  return cors_config->host_name_rule(s.c_str());
}

size_t cors_read_xml(void *ptr, size_t s, size_t n, void *ud){
  stringstream *ss = (stringstream *)ud;
  size_t len = ss->str().length();
  if(s*n < len){
    cout << "Cannot copy xml data, as len is not enough\n";
    return 0;
  }
  memcpy(ptr, (void *)ss->str().c_str(), len);
  return len;
}

void send_cors(set<string> o, set<string> h,
               list<string> e, uint8_t flags, 
               unsigned max_age){
  if(g_test->get_key_type() == KEY_TYPE_S3){
    RGWCORSRule rule(o, h, e, flags, max_age);
    RGWCORSConfiguration config;
    config.stack_rule(rule);
    stringstream ss;
    RGWCORSConfiguration_S3 *s3;
    s3 = static_cast<RGWCORSConfiguration_S3 *>(&config);
    s3->to_xml(ss);

    g_test->send_request(string("PUT"), string("/" S3_BUCKET_NAME "?cors"), cors_read_xml, 
                         (void *)&ss, ss.str().length());
  }else if(g_test->get_key_type() == KEY_TYPE_SWIFT){
    set<string>::iterator it;
    string a_o;
    for(it = o.begin(); it != o.end(); ++it){
      if(a_o.length() > 0)a_o.append(" ");
      a_o.append(*it);
    }
    g_test->set_extra_header(string("X-Container-Meta-Access-Control-Allow-Origin: ") + a_o);

    if(!h.empty()){
      string a_h;
      for(it = h.begin(); it != h.end(); ++it){
        if(a_h.length() > 0)a_h.append(" ");
        a_h.append(*it);
      }
      g_test->set_extra_header(string("X-Container-Meta-Access-Control-Allow-Headers: ") + a_h);
    }
    if(!e.empty()){
      string e_h;
      for(list<string>::iterator lit = e.begin(); lit != e.end(); ++lit){
        if(e_h.length() > 0)e_h.append(" ");
        e_h.append(*lit);
      }
      g_test->set_extra_header(string("X-Container-Meta-Access-Control-Expose-Headers: ") + e_h);
    }
    if(max_age != CORS_MAX_AGE_INVALID){
      char age[32];
      sprintf(age, "%u", max_age);
      g_test->set_extra_header(string("X-Container-Meta-Access-Control-Max-Age: ") + string(age));
    }
    //const char *data = "1";
    stringstream ss;
    ss << "1";
    g_test->send_request(string("POST"), string("/swift/v1/" SWIFT_BUCKET_NAME), cors_read_xml, 
                         (void *)&ss, 1);
  }
}

TEST(TestCORS, getcors_firsttime){
  if(g_test->get_key_type() == KEY_TYPE_SWIFT)return;
  ASSERT_EQ(0, create_bucket());
  g_test->send_request(string("GET"), string("/" S3_BUCKET_NAME "?cors"));
  EXPECT_EQ(404U, g_test->get_resp_code());
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, putcors_firsttime){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "example.com");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;

  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  /*Now get the CORS and check if its fine*/
  if(g_test->get_key_type() == KEY_TYPE_S3){
    g_test->send_request(string("GET"), string("/" S3_BUCKET_NAME "?cors"));
    EXPECT_EQ(200U, g_test->get_resp_code());

    RGWCORSRule *r = xml_to_cors_rule(string("example.com"));
    EXPECT_TRUE(r != NULL);
    if(!r)return;

    EXPECT_TRUE((r->get_allowed_methods() & (RGW_CORS_GET | RGW_CORS_PUT))
                == (RGW_CORS_GET | RGW_CORS_PUT));
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, putcors_invalid_hostname){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "*.example.*");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ((400U), g_test->get_resp_code());
  origins.erase(origins.begin(), origins.end());
 
  if((g_test->get_key_type() != KEY_TYPE_SWIFT)){
    origins.insert(origins.end(), "");
    send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
    EXPECT_EQ((400U), g_test->get_resp_code());
    origins.erase(origins.begin(), origins.end());
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, putcors_invalid_headers){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "www.example.com");
  h.insert(h.end(), "*-Header-*");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ((400U), g_test->get_resp_code());
  h.erase(h.begin(), h.end());
 
  if((g_test->get_key_type() != KEY_TYPE_SWIFT)){
    h.insert(h.end(), "");
    flags = RGW_CORS_GET | RGW_CORS_PUT;
    send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
    EXPECT_EQ((400U), g_test->get_resp_code());
    h.erase(h.begin(), h.end());
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_1){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "*.example.com");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;

  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: a.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: SomeHeader"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("a.example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0U, s.length());
    s = g_test->get_response(string("Access-Control-Max-Age"));
    EXPECT_EQ(0U, s.length());
    s = g_test->get_response(string("Access-Control-Expose-Headers"));
    EXPECT_EQ(0U, s.length());
  }

  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_2){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "*.example.com");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT | RGW_CORS_DELETE | RGW_CORS_HEAD;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: a.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: HEAD"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("a.example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("HEAD"));
  }

  g_test->set_extra_header(string("Origin: foo.bar.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: HEAD"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("foo.bar.example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("HEAD"));
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_3){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "*");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT | RGW_CORS_DELETE | RGW_CORS_HEAD;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  /*Check for HEAD in Access-Control-Allow-Methods*/
  g_test->set_extra_header(string("Origin: a.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: HEAD"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("a.example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("HEAD"));
  }

  /*Check for DELETE in Access-Control-Allow-Methods*/
  g_test->set_extra_header(string("Origin: foo.bar"));
  g_test->set_extra_header(string("Access-Control-Request-Method: DELETE"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("foo.bar"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("DELETE"));
  }

  /*Check for PUT in Access-Control-Allow-Methods*/
  g_test->set_extra_header(string("Origin: foo.bar"));
  g_test->set_extra_header(string("Access-Control-Request-Method: PUT"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("foo.bar"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("PUT"));
  }

  /*Check for POST in Access-Control-Allow-Methods*/
  g_test->set_extra_header(string("Origin: foo.bar"));
  g_test->set_extra_header(string("Access-Control-Request-Method: POST"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("foo.bar"));
    if(g_test->get_key_type() == KEY_TYPE_S3){
      s = g_test->get_response(string("Access-Control-Allow-Methods"));
      EXPECT_EQ(0U, s.length());
    }else{
      s = g_test->get_response(string("Access-Control-Allow-Methods"));
      EXPECT_EQ(0, s.compare("POST"));
    }
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_4){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "example.com");
  h.insert(h.end(), "Header1");
  h.insert(h.end(), "Header2");
  h.insert(h.end(), "*");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0U, s.length());
  }
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header1"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header1"));
  }
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header2"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header2"));
  }
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header2, Header1"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header2,Header1"));
  }
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header1, Header2"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header1,Header2"));
  }
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header1, Header2, Header3"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header1,Header2,Header3"));
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_5){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "example.com");
  e.insert(e.end(), "Expose1");
  e.insert(e.end(), "Expose2");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Expose-Headers"));
    EXPECT_EQ(0, s.compare("Expose1,Expose2"));
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_6){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;
  unsigned err = (g_test->get_key_type() == KEY_TYPE_SWIFT)?401U:403U;

  origins.insert(origins.end(), "http://www.example.com");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: http://example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: http://www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());

  origins.erase(origins.begin(), origins.end());
  origins.insert(origins.end(), "*.example.com");
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: .example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: http://example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: http://www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: https://www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());

  origins.erase(origins.begin(), origins.end());
  origins.insert(origins.end(), "https://example*.com");
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: https://example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: http://example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: www.example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(err, g_test->get_resp_code());

  g_test->set_extra_header(string("Origin: https://example.a.b.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, optionscors_test_options_7){
  ASSERT_EQ(0, create_bucket());
  set<string> origins, h;
  list<string> e;

  origins.insert(origins.end(), "example.com");
  h.insert(h.end(), "Header*");
  h.insert(h.end(), "Hdr-*-Length");
  h.insert(h.end(), "*-Length");
  h.insert(h.end(), "foo*foo");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());
  
  g_test->set_extra_header(string("Origin: example.com"));
  g_test->set_extra_header(string("Access-Control-Request-Method: GET"));
  g_test->set_extra_header(string("Access-Control-Allow-Headers: Header1, Header2, Header3, "
                                  "Hdr--Length, Hdr-1-Length, Header-Length, Content-Length, foofoofoo"));
  g_test->send_request(string("OPTIONS"), BUCKET_URL);
  EXPECT_EQ(200U, g_test->get_resp_code());
  if(g_test->get_resp_code() == 200){
    string s = g_test->get_response(string("Access-Control-Allow-Origin"));
    EXPECT_EQ(0, s.compare("example.com"));
    s = g_test->get_response(string("Access-Control-Allow-Methods"));
    EXPECT_EQ(0, s.compare("GET"));
    s = g_test->get_response(string("Access-Control-Allow-Headers"));
    EXPECT_EQ(0, s.compare("Header1,Header2,Header3,"
                           "Hdr--Length,Hdr-1-Length,Header-Length,Content-Length,foofoofoo"));
  }
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, deletecors_firsttime){
  if(g_test->get_key_type() == KEY_TYPE_SWIFT)return;
  ASSERT_EQ(0, create_bucket());
  g_test->send_request("DELETE", "/" S3_BUCKET_NAME "?cors");
  EXPECT_EQ(204U, g_test->get_resp_code());
  ASSERT_EQ(0, delete_bucket());
}

TEST(TestCORS, deletecors_test){
  set<string> origins, h;
  list<string> e;
  if(g_test->get_key_type() == KEY_TYPE_SWIFT)return;
  ASSERT_EQ(0, create_bucket());
  origins.insert(origins.end(), "example.com");
  uint8_t flags = RGW_CORS_GET | RGW_CORS_PUT;
  
  send_cors(origins, h, e, flags, CORS_MAX_AGE_INVALID);
  EXPECT_EQ(((g_test->get_key_type() == KEY_TYPE_SWIFT)?202U:200U), g_test->get_resp_code());

  g_test->send_request("GET", "/" S3_BUCKET_NAME "?cors");
  EXPECT_EQ(200U, g_test->get_resp_code());
  g_test->send_request("DELETE", "/" S3_BUCKET_NAME "?cors");
  EXPECT_EQ(204U, g_test->get_resp_code());
  g_test->send_request("GET", "/" S3_BUCKET_NAME "?cors");
  EXPECT_EQ(404U, g_test->get_resp_code());
  ASSERT_EQ(0, delete_bucket());
}

int main(int argc, char *argv[]){
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_test = new test_cors_helper();
  finisher = new Finisher(g_ceph_context);
#ifdef GTEST
  ::testing::InitGoogleTest(&argc, argv);
#endif
  finisher->start();

  if(g_test->extract_input((unsigned)argc, argv) < 0){
    print_usage(argv[0]);
    return -1;
  }
#ifdef GTEST
  int r = RUN_ALL_TESTS();
  if (r >= 0) {
    cout << "There are failures in the test case\n";
    return -1;
  }
#endif
  finisher->stop();
  delete g_test;
  delete finisher;
  return 0;
}

