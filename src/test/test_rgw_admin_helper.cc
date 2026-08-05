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

#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/wait.h>
#include <unistd.h>
#include "common/ceph_crypto.h"
#include "common/ceph_json.h"
#include "common/code_environment.h"
#include "common/armor.h"
#include "include/str_list.h"
#include "test_rgw_admin_helper.h"

using namespace std;

namespace admin_helper
{
    test_helper *g_test;

    void print_usage(char *exec)
    {
        cout << "Usage: " << exec << " <Options>\n";
        cout << "Options:\n"
                "-g <gw-ip> - The ip address of the gateway\n"
                "-p <gw-port> - The port number of the gateway\n"
                "-c <ceph.conf> - Absolute path of ceph config file\n"
                "-rgw-admin <path/to/radosgw-admin> - radosgw-admin absolute path\n";
    }

    test_helper::test_helper() : curl_inst(0), resp_data(NULL), resp_code(0)
    {
        curl_global_init(CURL_GLOBAL_ALL);
    }

    test_helper::~test_helper()
    {
        curl_global_cleanup();
    }

    int test_helper::send_request(string method, string res,
                                  size_t (*read_function)(void *, size_t, size_t, void *),
                                  void *ud,
                                  size_t length)
    {
        string url;
        string auth, date;
        url.append(string("http://") + host);
        if (port.length() > 0)
            url.append(string(":") + port);
        url.append(res);
        curl_inst = curl_easy_init();
        if (curl_inst)
        {
            curl_easy_setopt(curl_inst, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl_inst, CURLOPT_CUSTOMREQUEST, method.c_str());
            curl_easy_setopt(curl_inst, CURLOPT_VERBOSE, CURL_VERBOSE);
            curl_easy_setopt(curl_inst, CURLOPT_HEADERFUNCTION, admin_helper::write_header);
            curl_easy_setopt(curl_inst, CURLOPT_WRITEHEADER, (void *)this);
            curl_easy_setopt(curl_inst, CURLOPT_WRITEFUNCTION, admin_helper::write_data);
            curl_easy_setopt(curl_inst, CURLOPT_WRITEDATA, (void *)this);
            if (read_function)
            {
                curl_easy_setopt(curl_inst, CURLOPT_READFUNCTION, read_function);
                curl_easy_setopt(curl_inst, CURLOPT_READDATA, (void *)ud);
                curl_easy_setopt(curl_inst, CURLOPT_UPLOAD, 1L);
                curl_easy_setopt(curl_inst, CURLOPT_INFILESIZE_LARGE, (curl_off_t)length);
            }

            get_date(date);
            string http_date;
            http_date.append(string("Date: ") + date);

            string s3auth;
            if (admin_helper::get_s3_auth(method, creds, date, res, s3auth) < 0)
                return -1;
            auth.append(string("Authorization: AWS ") + s3auth);

            struct curl_slist *slist = NULL;
            slist = curl_slist_append(slist, auth.c_str());
            slist = curl_slist_append(slist, http_date.c_str());
            for (list<string>::iterator it = extra_hdrs.begin();
                 it != extra_hdrs.end(); ++it)
            {
                slist = curl_slist_append(slist, (*it).c_str());
            }
            if (read_function)
                curl_slist_append(slist, "Expect:");
            curl_easy_setopt(curl_inst, CURLOPT_HTTPHEADER, slist);

            response.erase(response.begin(), response.end());
            extra_hdrs.erase(extra_hdrs.begin(), extra_hdrs.end());
            CURLcode res = curl_easy_perform(curl_inst);
            if (res != CURLE_OK)
            {
                cout << "Curl perform failed for " << url << ", res: " << curl_easy_strerror(res) << "\n";
                return -1;
            }
            curl_slist_free_all(slist);
        }
        curl_easy_cleanup(curl_inst);
        return 0;
    }

    string &test_helper::get_response(string hdr)
    {
        return response[hdr];
    }

    void test_helper::set_extra_header(string hdr)
    {
        extra_hdrs.push_back(hdr);
    }

    void test_helper::set_response(char *r)
    {
        string sr(r), h, v;
        size_t off = sr.find(": ");
        if (off != string::npos)
        {
            h.assign(sr, 0, off);
            v.assign(sr, off + 2, sr.find("\r\n") - (off + 2));
        }
        else
        {
            /*Could be the status code*/
            if (sr.find("HTTP/") != string::npos)
            {
                h.assign(HTTP_RESPONSE_STR);
                off = sr.find(" ");
                v.assign(sr, off + 1, sr.find("\r\n") - (off + 1));
                resp_code = atoi((v.substr(0, 3)).c_str());
            }
        }
        response[h] = v;
    }

    void test_helper::set_response_data(char *data, size_t len)
    {
        if (resp_data)
            delete resp_data;
        resp_data = new string(data, len);
    }
    string &test_helper::get_rgw_admin_path()
    {
        return rgw_admin_path;
    }
    string &test_helper::get_ceph_conf_path()
    {
        return conf_path;
    }
    void test_helper::set_creds(string &c)
    {
        creds = c;
    }
    const string *test_helper::get_response_data() { return resp_data; }
    unsigned test_helper::get_resp_code() { return resp_code; }

    int test_helper::extract_input(int argc, char *argv[])
    {
#define ERR_CHECK_NEXT_PARAM(o)  \
    if (((int)loop + 1) >= argc) \
        return -1;               \
    else                         \
        o = argv[loop + 1];

        for (unsigned loop = 1; loop < (unsigned)argc; loop += 2)
        {
            if (strcmp(argv[loop], "-g") == 0)
            {
                ERR_CHECK_NEXT_PARAM(host);
            }
            else if (strcmp(argv[loop], "-p") == 0)
            {
                ERR_CHECK_NEXT_PARAM(port);
            }
            else if (strcmp(argv[loop], "-c") == 0)
            {
                ERR_CHECK_NEXT_PARAM(conf_path);
            }
            else if (strcmp(argv[loop], "-rgw-admin") == 0)
            {
                ERR_CHECK_NEXT_PARAM(rgw_admin_path);
            }
            else
                return -1;
        }
        if (host.empty() || rgw_admin_path.empty())
            return -1;
        return 0;
    }

    size_t write_header(void *ptr, size_t size, size_t nmemb, void *ud)
    {
        test_helper *h = static_cast<test_helper *>(ud);
        h->set_response((char *)ptr);
        return size * nmemb;
    }

    size_t write_data(void *ptr, size_t size, size_t nmemb, void *ud)
    {
        test_helper *h = static_cast<test_helper *>(ud);
        h->set_response_data((char *)ptr, size * nmemb);
        return size * nmemb;
    }

    inline void buf_to_hex(const unsigned char *buf, int len, char *str)
    {
        int i;
        str[0] = '\0';
        for (i = 0; i < len; i++)
        {
            sprintf(&str[i * 2], "%02x", (int)buf[i]);
        }
    }

    void calc_hmac_sha1(const char *key, int key_len,
                        const char *msg, int msg_len, char *dest)
    /* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */
    {
        ceph::crypto::HMACSHA1 hmac((const unsigned char *)key, key_len);
        hmac.Update((const unsigned char *)msg, msg_len);
        hmac.Final((unsigned char *)dest);

        char hex_str[(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2) + 1];
        admin_helper::buf_to_hex((unsigned char *)dest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, hex_str);
    }

    int get_s3_auth(const string &method, string creds, const string &date, string res, string &out)
    {
        string aid, secret, auth_hdr;
        string tmp_res;
        size_t off = creds.find(":");
        out = "";
        if (off != string::npos)
        {
            aid.assign(creds, 0, off);
            secret.assign(creds, off + 1, string::npos);

            /*sprintf(auth_hdr, "%s\n\n\n%s\n%s", req_type, date, res);*/
            char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
            char b64[65]; /* 64 is really enough */
            size_t off = res.find("?");
            if (off == string::npos)
                tmp_res = res;
            else
                tmp_res.assign(res, 0, off);
            auth_hdr.append(method + string("\n\n\n") + date + string("\n") + tmp_res);
            admin_helper::calc_hmac_sha1(secret.c_str(), secret.length(),
                                         auth_hdr.c_str(), auth_hdr.length(), hmac_sha1);
            int ret = ceph_armor(b64, b64 + 64, hmac_sha1,
                                 hmac_sha1 + CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
            if (ret < 0)
            {
                cout << "ceph_armor failed\n";
                return -1;
            }
            b64[ret] = 0;
            out.append(aid + string(":") + b64);
        }
        else
            return -1;
        return 0;
    }

    void get_date(string &d)
    {
        struct timeval tv;
        char date[64];
        struct tm tm;
        char *days[] = {(char *)"Sun", (char *)"Mon", (char *)"Tue",
                        (char *)"Wed", (char *)"Thu", (char *)"Fri",
                        (char *)"Sat"};
        char *months[] = {(char *)"Jan", (char *)"Feb", (char *)"Mar",
                          (char *)"Apr", (char *)"May", (char *)"Jun",
                          (char *)"Jul", (char *)"Aug", (char *)"Sep",
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

    int run_rgw_admin(string &cmd, string &resp)
    {
        pid_t pid;
        pid = fork();
        if (pid == 0)
        {
            /* child */
            list<string> l;
            get_str_list(cmd, " \t", l);
            char *argv[l.size()];
            unsigned loop = 1;

            argv[0] = (char *)"radosgw-admin";
            for (list<string>::iterator it = l.begin();
                 it != l.end(); ++it)
            {
                argv[loop++] = (char *)(*it).c_str();
            }
            argv[loop] = NULL;
            if (!freopen(RGW_ADMIN_RESP_PATH, "w+", stdout))
            {
                cout << "Unable to open stdout file" << std::endl;
            }
            execv((g_test->get_rgw_admin_path()).c_str(), argv);
        }
        else if (pid > 0)
        {
            int status;
            waitpid(pid, &status, 0);
            if (WIFEXITED(status))
            {
                if (WEXITSTATUS(status) != 0)
                {
                    cout << "Child exited with status " << WEXITSTATUS(status) << std::endl;
                    return -1;
                }
            }
            ifstream in;
            struct stat st;

            if (stat(RGW_ADMIN_RESP_PATH, &st) < 0)
            {
                cout << "Error stating the admin response file, errno " << errno << std::endl;
                return -1;
            }
            else
            {
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
        }
        else
            return -1;
        return 0;
    }

    int get_creds(string &json, string &creds)
    {
        JSONParser parser;
        if (!parser.parse(json.c_str(), json.length()))
        {
            cout << "Error parsing create user response" << std::endl;
            return -1;
        }

        RGWUserInfo info;
        decode_json_obj(info, &parser);
        creds = "";
        for (map<string, RGWAccessKey>::iterator it = info.access_keys.begin();
             it != info.access_keys.end(); ++it)
        {
            RGWAccessKey _k = it->second;
            /*cout << "accesskeys [ " << it->first << " ] = " <<
              "{ " << _k.id << ", " << _k.key << ", " << _k.subuser << "}" << std::endl;*/
            creds.append(it->first + string(":") + _k.key);
            break;
        }
        return 0;
    }

    int account_create(string &account_id, string &account_name)
    {
        stringstream ss;
        ss << "-c " << g_test->get_ceph_conf_path() << " account create --account-id=" << account_id << " --account-name=" << account_name;
        string cmd = ss.str();
        string out;
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error creating account" << std::endl;
            return -1;
        }
        return 0;
    }

    int user_create(string &uid, string &display_name, bool set_creds, const string &account_id, bool is_admin)
    {
        stringstream ss;
        string creds;
        ss << "-c " << g_test->get_ceph_conf_path() << " user create --uid=" << uid
           << " --display-name=" << display_name;
        if (!account_id.empty()){
            ss << " --account-id=" << account_id;
        }
        if (is_admin)
        {
            ss << " --admin";
        }
        string out;
        string cmd = ss.str();
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error creating user" << std::endl;
            return -1;
        }
        get_creds(out, creds);
        if (set_creds)
            g_test->set_creds(creds);
        return 0;
    }

    int account_info(string &account_id, RGWAccountInfo &a_info)
    {
        stringstream ss;
        ss << "-c " << g_test->get_ceph_conf_path() << " account get --account-id=" << account_id;
        string cmd = ss.str();
        string out;
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error reading account information" << std::endl;
            return -1;
        }
        JSONParser parser;
        if (!parser.parse(out.c_str(), out.length()))
        {
            cout << "Error parsing account info response" << std::endl;
            return -1;
        }
        decode_json_obj(a_info, &parser);
        return 0;
    }

    int user_info(string &uid, string &display_name, RGWUserInfo &uinfo)
    {
        stringstream ss;
        ss << "-c " << g_test->get_ceph_conf_path() << " user info --uid=" << uid
           << " --display-name=" << display_name;

        string out;
        string cmd = ss.str();
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error reading user information" << std::endl;
            return -1;
        }
        JSONParser parser;
        if (!parser.parse(out.c_str(), out.length()))
        {
            cout << "Error parsing create user response" << std::endl;
            return -1;
        }
        decode_json_obj(uinfo, &parser);
        return 0;
    }

    int account_rm(string &account_id)
    {
        stringstream ss;
        ss << "-c " << g_test->get_ceph_conf_path() << " account rm --account-id=" << account_id;
        string cmd = ss.str();
        string out;
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error removing account" << std::endl;
            return -1;
        }
        return 0;
    }

    int user_rm(string &uid, string &display_name)
    {
        stringstream ss;
        ss << "-c " << g_test->get_ceph_conf_path() << " user rm --uid=" << uid
           << " --display-name=" << display_name;

        string out;
        string cmd = ss.str();
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error removing user" << std::endl;
            return -1;
        }
        return 0;
    }

    int caps_add(const string caps_name, const string uid, const char *perm)
    {
        stringstream ss;

        ss << "-c " << g_test->get_ceph_conf_path() << " caps add --caps=" << caps_name << "=" << perm << " --uid=" << uid;
        string out;
        string cmd = ss.str();
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error adding caps to user" << std::endl;
            return -1;
        }
        return 0;
    }

    int caps_rm(const string caps_name, const string uid, const char *perm)
    {
        stringstream ss;

        ss << "-c " << g_test->get_ceph_conf_path() << " caps rm --caps=" << caps_name << "=" << perm << " --uid=" << uid;
        string out;
        string cmd = ss.str();
        if (run_rgw_admin(cmd, out) != 0)
        {
            cout << "Error removing caps from user" << std::endl;
            return -1;
        }
        return 0;
    }

    int compare_access_keys(RGWAccessKey &k1, RGWAccessKey &k2)
    {
        if (k1.id.compare(k2.id) != 0)
            return -1;
        if (k1.key.compare(k2.key) != 0)
            return -1;
        if (k1.subuser.compare(k2.subuser) != 0)
            return -1;

        return 0;
    }

    int compare_user_info(RGWUserInfo &i1, RGWUserInfo &i2, const string meta_caps)
    {
        int rv;

        if ((rv = i1.user_id.id.compare(i2.user_id.id)) != 0)
            return rv;
        if ((rv = i1.display_name.compare(i2.display_name)) != 0)
            return rv;
        if ((rv = i1.user_email.compare(i2.user_email)) != 0)
            return rv;
        if (i1.access_keys.size() != i2.access_keys.size())
            return -1;
        for (map<string, RGWAccessKey>::iterator it = i1.access_keys.begin();
             it != i1.access_keys.end(); ++it)
        {
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
             it != i1.swift_keys.end(); ++it)
        {
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
             it != i1.subusers.end(); ++it)
        {
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

    size_t read_dummy_post(void *ptr, size_t s, size_t n, void *ud)
    {
        int dummy = 0;
        memcpy(ptr, &dummy, sizeof(dummy));
        return sizeof(dummy);
    }

    int parse_json_resp(JSONParser &parser)
    {
        string *resp;
        resp = (string *)g_test->get_response_data();
        if (!resp)
            return -1;
        if (!parser.parse(resp->c_str(), resp->length()))
        {
            cout << "Error parsing create user response" << std::endl;
            return -1;
        }
        return 0;
    }

    size_t meta_read_json(void *ptr, size_t s, size_t n, void *ud)
    {
        stringstream *ss = (stringstream *)ud;
        size_t len = ss->str().length();
        if (s * n < len)
        {
            cout << "Cannot copy json data, as len is not enough\n";
            return 0;
        }
        memcpy(ptr, (void *)ss->str().c_str(), len);
        return len;
    }
};
