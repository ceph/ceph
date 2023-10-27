// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_perf_counters.h"
#include "common/Clock.h"
#include "rgw_common.h"
#include "rgw_arn.h"
#include "rgw_op.h"

using namespace std;


class RGWExternIAMAuthorize {
private:
    CephContext *cct;
    vector<pair<string, string>> headers;

    RGWExternIAMAuthorize(CephContext *cct) : cct(cct)
    {
        init_headers();

        ceph_assert(validate());
    }

    void init_headers() {
        headers.push_back(make_pair("Authorization", cct->_conf->rgw_extern_iam_authorization_header));
        headers.push_back(make_pair("Content-Type", "application/json"));
    }

    bool validate() {
        if (addr() == "") {
            cerr << "EXTERN_IAM_ADDR not provided" << std::endl;
            return false;
        }

        return true;
    }

    inline void measure_latency(utime_t& start_time) {
        perfcounter->tinc(l_rgw_extern_iam_lat, (ceph_clock_now() - start_time));
    }

public:
    RGWExternIAMAuthorize(const RGWExternIAMAuthorize& obj) = delete;
    void operator=(const RGWExternIAMAuthorize &) = delete;

    static RGWExternIAMAuthorize& get_instance(CephContext *cct) {
        static RGWExternIAMAuthorize instance(cct);
        return instance;
    }

    const string& addr() {
        return cct->_conf->rgw_extern_iam_addr;
    }

    const bool verify_ssl() {
        return cct->_conf->rgw_extern_iam_verify_ssl;
    }

    const string& unix_socket() {
        return cct->_conf->rgw_extern_iam_unix_sock_path;
    }

    int eval(const DoutPrefixProvider *dpp,
             const rgw::IAM::Environment& env,
             const rgw::ARN& resource, uint64_t op,
             boost::optional<const rgw::auth::Identity&> ida,
             rgw::IAM::Effect& effect);
};
