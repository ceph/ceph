// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020-2021 IBM Research Europe <koutsovasilis.panagiotis1@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef RGW_S3_MIRROR_LOCAL_H
#define RGW_S3_MIRROR_LOCAL_H

#include <string.h>

#include "rgw_client_io.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_acl_s3.h"
#include "rgw_sal_rados.h"
#include "rgw_request.h"
#include "rgw_op.h"
#include "rgw_common.h"
#include "services/svc_zone_utils.h"

#include <aws/core/Aws.h>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

static inline string make_uri(const string &bucket_name, const string &object_name)
{
    string uri("/");
    uri.reserve(bucket_name.length() + object_name.length() + 2);
    uri += bucket_name;
    uri += "/";
    uri += object_name;
    return uri;
}

// S3MirrorIO is derived from RGWLibIO customized for the needs of S3Mirror
class S3MirrorIO : public rgw::io::BasicClient,
    public rgw::io::Accounter
{
    RGWUserInfo user_info;
    RGWEnv env;

public:
    S3MirrorIO()
    {
        get_env().set("HTTP_HOST", "");
    }
    explicit S3MirrorIO(const RGWUserInfo &_user_info)
        : user_info(_user_info) {}

    int init_env(CephContext *cct) override
    {
        env.init(cct);
        env.set("HTTP_S3MIRROR", "bypass"); //bypass the S3 Mirror interception
        return 0;
    }

    const RGWUserInfo &get_user()
    {
        return user_info;
    }

    int set_uid(rgw::sal::RGWRadosStore *store, const rgw_user &uid);

    int write_data(const char *buf, int len);
    int read_data(char *buf, int len);
    int send_status(int status, const char *status_name);
    int send_100_continue();
    int complete_header();
    int send_content_length(uint64_t len);

    RGWEnv &get_env() noexcept override
    {
        return env;
    }

    size_t complete_request() override
    { /* XXX */
        return 0;
    };

    void set_account(bool) override
    {
        return;
    }

    uint64_t get_bytes_sent() const override
    {
        return 0;
    }

    uint64_t get_bytes_received() const override
    {
        return 0;
    }

}; /* S3MirrorIO */

// RGWRESTMgr_S3Mirror is derived from RGWRESTMgr_Lib customized for the needs of S3Mirror
class RGWRESTMgr_S3Mirror : public RGWRESTMgr
{
public:
    RGWRESTMgr_S3Mirror() {}
    ~RGWRESTMgr_S3Mirror() override {}
};

// RGWHandler_S3Mirror is derived from RGWHandler_Lib customized for the needs of S3Mirror
class RGWHandler_S3Mirror : public RGWHandler
{
    friend class RGWRESTMgr_S3Mirror;

public:
    int authorize(const DoutPrefixProvider *dpp) override;

    RGWHandler_S3Mirror() {}
    ~RGWHandler_S3Mirror() override {}
    static int init_from_header(rgw::sal::RGWRadosStore *_store, struct req_state *s);
};

// S3MirrorRequest is derived from RGWLibRequest customized for the needs of S3Mirror
class S3MirrorRequest : public RGWRequest,
    public RGWHandler_S3Mirror
{
private:
    std::unique_ptr<rgw::sal::RGWUser> tuser;
public:
    CephContext *cct;
    rgw::sal::RGWRadosStore *store;
    boost::optional<RGWSysObjectCtx> sysobj_ctx;

    /* unambiguiously return req_state */
    inline struct req_state *get_state() {
        return this->RGWRequest::s;
    }

    S3MirrorRequest(CephContext *_cct, std::unique_ptr<rgw::sal::RGWUser> _user, rgw::sal::RGWRadosStore *_store)
        : RGWRequest(_store->getRados()->get_new_req_id()), tuser(std::move(_user)),
        cct(_cct), store(_store)
    {
    }

    int postauth_init() override {
        return 0;
    }

    /* descendant equivalent of *REST*::init_from_header(...):
     * prepare request for execute()--should mean, fixup URI-alikes
     * and any other expected stat vars in local req_state, for
     * now */
    virtual int header_init() = 0;

    /* descendant initializer responsible to call RGWOp::init()--which
     * descendants are required to inherit */
    virtual int op_init() = 0;

    using RGWHandler::init;

    int init(const RGWEnv &rgw_env, RGWObjectCtx *rados_ctx,
        S3MirrorIO *io, struct req_state *_s)
    {

        RGWRequest::init_state(_s);
        RGWHandler::init(rados_ctx->get_store(), _s, io);

        sysobj_ctx.emplace(store->svc()->sysobj);

        get_state()->obj_ctx = rados_ctx;
        get_state()->sysobj_ctx = &(sysobj_ctx.get());
        get_state()->req_id = store->svc()->zone_utils->unique_id(id);
        get_state()->trans_id = store->svc()->zone_utils->unique_trans_id(id);
        get_state()->bucket_tenant = tuser->get_tenant();
        get_state()->set_user(tuser);

        int ret = header_init();
        if (ret == 0)
        {
            ret = init_from_header(rados_ctx->get_store(), _s);
        }
        return ret;
    }

    virtual bool only_bucket() = 0;

    int read_permissions(RGWOp *op) override;

};

// Request to write an object locally by reading an Aws::IOStream
class S3MirrorPutObjRequest : public S3MirrorRequest,
    public RGWPutObj /* RGWOp */
{
public:
    const string &bucket_name;
    const string &obj_name;
    size_t bytes_written;
    char * buffer;
    uint64_t content_length;

    S3MirrorPutObjRequest(CephContext *_cct, rgw::sal::RGWRadosStore *_store,
        std::unique_ptr<rgw::sal::RGWUser> _user, const string &_bname, const string &_oname,
        char * object_buffer, long long object_length)
        : S3MirrorRequest(_cct, std::move(_user), _store), bucket_name(_bname), obj_name(_oname),
        bytes_written(0), buffer(object_buffer), content_length(object_length)
    {
        op = this;
    }

    bool only_bucket() override {
        return true;
    }

    int op_init() override
    {
        // assign store, s, and dialect_handler
        RGWObjectCtx *rados_ctx = static_cast<RGWObjectCtx *>(get_state()->obj_ctx);
        // framework promises to call op_init after parent init
        ceph_assert(rados_ctx);
        RGWOp::init(rados_ctx->get_store(), get_state(), this);
        op = this; // assign self as op: REQUIRED

        int rc = valid_s3_object_name(obj_name);
        if (rc != 0)
            return rc;

        return 0;
    }

    int header_init() override
    {
        struct req_state *s = get_state();
        s->info.method = "PUT";
        s->op = OP_PUT;

        /* XXX derp derp derp */
        string uri = make_uri(bucket_name, obj_name);
        s->relative_uri = uri;
        s->info.request_uri = uri; // XXX
        s->info.effective_uri = uri;
        s->info.request_params = "";
        s->info.domain = ""; /* XXX ? */

        /* XXX required in RGWOp::execute() */
        s->content_length = content_length;

        return 0;
    }

    int get_params() override
    {
        struct req_state *s = get_state();
        RGWAccessControlPolicy_S3 s3policy(s->cct);
        /* we don't have (any) headers, so just create canned ACLs */
        int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
        policy = s3policy;
        return ret;
    }

    int get_data(ceph::bufferlist &_bl) override
    {
      // std::this_thread::sleep_for(std::chrono::seconds(10));
      struct req_state *s = get_state();
      size_t cl;
      uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
      cl = content_length - ofs;
      if (cl > chunk_size)
        cl = chunk_size;

      _bl.append(&(this->buffer[bytes_written]), cl);

      bytes_written += cl;
      return cl;
    }

    void send_response() override {}

    int verify_params() override
    {
        if (content_length > cct->_conf->rgw_max_put_size)
            return -ERR_TOO_LARGE;
        return 0;
    }

    ceph::bufferlist *get_attr(const string &k)
    {
        auto iter = attrs.find(k);
        return (iter != attrs.end()) ? &(iter->second) : nullptr;
    }

}; /* S3MirrorPutObjRequest */

// Request to read a local object and write to an Aws::IOStream
class S3MirrorGetObjRequest : public S3MirrorRequest,
    public RGWGetObj /* RGWOp */
{
public:
    const string &bucket_name;
    const string &obj_name;
    const shared_ptr<Aws::IOStream> *body_stream;
    uint64_t content_length;

    S3MirrorGetObjRequest(CephContext *_cct, rgw::sal::RGWRadosStore *_store,
        std::unique_ptr<rgw::sal::RGWUser> _user, const string &_bname, const string &_oname,
        const shared_ptr<Aws::IOStream> *_body_stream)
        : S3MirrorRequest(_cct, std::move(_user), _store), bucket_name(_bname), obj_name(_oname),
        body_stream(_body_stream)
    {
        op = this;

        /* fixup RGWGetObj (already know range parameters) */
        RGWGetObj::range_parsed = false;
        RGWGetObj::get_data = true; // XXX
        RGWGetObj::partial_content = false;
    }

    bool only_bucket() override {
        return false;
    }

    int op_init() override
    {
        // assign store, s, and dialect_handler
        RGWObjectCtx *rados_ctx = static_cast<RGWObjectCtx *>(get_state()->obj_ctx);
        // framework promises to call op_init after parent init
        ceph_assert(rados_ctx);
        RGWOp::init(rados_ctx->get_store(), get_state(), this);
        op = this; // assign self as op: REQUIRED
        return 0;
    }

    int header_init() override
    {

        struct req_state *s = get_state();
        s->info.method = "GET";
        s->op = OP_GET;

        /* XXX derp derp derp */
        string uri = make_uri(bucket_name, obj_name);
        s->relative_uri = uri;
        s->info.request_uri = uri; // XXX
        s->info.effective_uri = uri;
        s->info.request_params = "";
        s->info.domain = ""; /* XXX ? */

        return 0;
    }

    int get_params() override
    {
        return 0;
    }

    int send_response_data(ceph::buffer::list &bl, off_t bl_off,
        off_t bl_len) override
    {
        for (auto &bp : bl.buffers())
        {
            /* if for some reason bl_off indicates the start-of-data is not at
            * the current buffer::ptr, skip it and account */
            if (bl_off > bp.length())
            {
                bl_off -= bp.length();
                continue;
            }

            (*this->body_stream)->write(bp.c_str(), bp.length());
        }
        return 0;
    }

    int send_response_data_error() override
    {
        /* S3 implementation just sends nothing--there is no side effect
     * to simulate here */
        return 0;
    }
}; /* S3MirrorGetObjRequest */


// Request to create a bucket locally  
class S3MirrorCreateBucketRequest : public S3MirrorRequest,
    public RGWCreateBucket /* RGWOp */
{
public:
    const string &bucket_name;

    S3MirrorCreateBucketRequest(CephContext *_cct, rgw::sal::RGWRadosStore *_store,
        std::unique_ptr<rgw::sal::RGWUser> _user, string &_bname)
        : S3MirrorRequest(_cct, std::move(_user), _store), bucket_name(_bname)
    {
        op = this;
    }

    bool only_bucket() override {
        return false;
    }

    int read_permissions(RGWOp *op_obj) override
    {
        /* we ARE a 'create bucket' request (cf. rgw_rest.cc, ll. 1305-6) */
        return 0;
    }

    int op_init() override
    {
        // assign store, s, and dialect_handler
        RGWObjectCtx *rados_ctx = static_cast<RGWObjectCtx *>(get_state()->obj_ctx);
        // framework promises to call op_init after parent init
        ceph_assert(rados_ctx);
        RGWOp::init(rados_ctx->get_store(), get_state(), this);
        op = this; // assign self as op: REQUIRED
        return 0;
    }

    int header_init() override
    {

        struct req_state *s = get_state();
        s->info.method = "PUT";
        s->op = OP_PUT;

        string uri = "/" + bucket_name;
        /* XXX derp derp derp */
        s->relative_uri = uri;
        s->info.request_uri = uri; // XXX
        s->info.effective_uri = uri;
        s->info.request_params = "";
        s->info.domain = ""; /* XXX ? */

        return 0;
    }

    int get_params() override
    {
        struct req_state *s = get_state();
        RGWAccessControlPolicy_S3 s3policy(s->cct);
        /* we don't have (any) headers, so just create canned ACLs */
        int ret = s3policy.create_canned(s->owner, s->bucket_owner, s->canned_acl);
        policy = s3policy;
        return ret;
    }

    void send_response() override
    {
        /* TODO: something (maybe) */
    }
}; /* S3MirrorCreateBucketRequest */

int do_process_request(rgw::sal::RGWRadosStore *store, S3MirrorRequest *req);

#endif