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

#include <map>
#include <list>
extern "C"{
#include <curl/curl.h>
}
#include <string.h>
#include "rgw_common.h"

using namespace std;

#define CURL_VERBOSE 0
#define HTTP_RESPONSE_STR "RespCode"
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20
#define RGW_ADMIN_RESP_PATH "/tmp/.test_rgw_admin_resp"
#define CEPH_UID "ceph"

namespace admin_helper
{
    class test_helper
    {
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
        test_helper();
        ~test_helper();
        int send_request(string method, string uri,
                         size_t (*function)(void *, size_t, size_t, void *) = 0,
                         void *ud = 0, size_t length = 0);
        int extract_input(int argc, char *argv[]);
        string &get_response(string hdr);
        void set_extra_header(string hdr);
        void set_response(char *val);
        void set_response_data(char *data, size_t len);
        string &get_rgw_admin_path();
        string &get_ceph_conf_path();
        void set_creds(string &c);
        const string *get_response_data();
        unsigned get_resp_code();
    };

    size_t write_header(void *ptr, size_t size, size_t nmemb, void *ud);
    size_t write_data(void *ptr, size_t size, size_t nmemb, void *ud);
    inline void buf_to_hex(const unsigned char *buf, int len, char *str);
    void calc_hmac_sha1(const char *key, int key_len,
                        const char *msg, int msg_len, char *dest);
    int get_s3_auth(const string &method, string creds, const string &date, string res, string &out);
    void get_date(string &d);
    void print_usage(char *exec);
    int run_rgw_admin(string &cmd, string &resp);
    int account_create(string &account_id, string &account_name);
    int user_create(string &uid, string &display_name, bool set_creds = true, const string &account_id = "", bool is_admin = false);
    int account_info(string &account_id, RGWAccountInfo &a_info);
    int user_info(string &uid, string &display_name, RGWUserInfo &uinfo);
    int account_rm(string &account_id);
    int user_rm(string &uid, string &display_name);
    int get_creds(string &json, string &creds);
    int caps_add(const string caps_name, const string uid, const char *perm);
    int caps_rm(const string caps_name, const string uid, const char *perm);
    int compare_access_keys(RGWAccessKey &k1, RGWAccessKey &k2);
    int compare_user_info(RGWUserInfo &i1, RGWUserInfo &i2, const string meta_caps);
    size_t read_dummy_post(void *ptr, size_t s, size_t n, void *ud);
    int parse_json_resp(JSONParser &parser);
    size_t meta_read_json(void *ptr, size_t s, size_t n, void *ud);
    extern admin_helper::test_helper *g_test;
};
