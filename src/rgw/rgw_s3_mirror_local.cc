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

#include "rgw_s3_mirror_local.h"

int RGWHandler_S3Mirror::authorize(const DoutPrefixProvider *dpp)
{
    /* TODO: handle
     *  1. subusers
     *  2. anonymous access
     *  3. system access
     *  4. ?
     *
     *  Much or all of this depends on handling the cached authorization
     *  correctly (e.g., dealing with keystone) at mount time.
     */
    s->perm_mask = RGW_PERM_FULL_CONTROL;

    // populate the owner info
    s->owner.set_id(s->user->get_id());
    s->owner.set_name(s->user->get_display_name());

    return 0;
}

int RGWHandler_S3Mirror::init_from_header(rgw::sal::RGWRadosStore *store, struct req_state *s)
{
    string req;
    string first;

    const char *req_name = s->relative_uri.c_str();
    const char *p;

    /* skip request_params parsing, rgw_file should not be
     * seeing any */
    if (*req_name == '?')
    {
        p = req_name;
    }
    else
    {
        p = s->info.request_params.c_str();
    }

    s->info.args.set(p);
    s->info.args.parse();

    if (*req_name != '/')
        return 0;

    req_name++;

    if (!*req_name)
        return 0;

    req = req_name;
    int pos = req.find('/');
    if (pos >= 0)
    {
        first = req.substr(0, pos);
    }
    else
    {
        first = req;
    }
    
    if (s->bucket_name.empty())
    {
        s->bucket_name = move(first);
        if (pos >= 0)
        {
            
            // XXX ugh, another copy
            string encoded_obj_str = req.substr(pos + 1);
            s->object = store->get_object(rgw_obj_key(encoded_obj_str, s->info.args.get("versionId")));
        }
    }
    else
    {
        s->object = store->get_object(rgw_obj_key(req_name, s->info.args.get("versionId")));
    }
    return 0;
}

int S3MirrorRequest::read_permissions(RGWOp *op)
{
    /* bucket and object ops */
    int ret = rgw_build_bucket_policies(this->store, get_state());
    if (ret < 0)
    {
        if (ret == -ENODATA)
            ret = -EACCES;
    }
    else if (!only_bucket())
    {
        /* object ops */
        ret = rgw_build_object_policies(store, get_state(),
                                        op->prefetch_data());
        if (ret < 0)
        {
            if (ret == -ENODATA)
                ret = -EACCES;
        }
    }
    return ret;
}

int do_process_request(rgw::sal::RGWRadosStore *store, S3MirrorRequest *req)
{
  S3MirrorIO io;
  int ret = 0;

  /*
    * invariant: valid requests are derived from RGWOp--well-formed
    * requests should have assigned RGWRequest::op in their descendant
    * constructor--if not, the compiler can find it, at the cost of
    * a runtime check
    */
  RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp *>(req);
  if (!op)
  {
      return -EINVAL;
  }

  io.init(req->cct);

  RGWEnv &rgw_env = io.get_env();

  /* XXX
    * until major refactoring of req_state and req_info, we need
    * to build their RGWEnv boilerplate from the RGWLibRequest,
    * pre-staging any strings (HTTP_HOST) that provoke a crash when
    * not found
    */

  /* XXX for now, use "";  could be a legit hostname, or, in future,
    * perhaps a tenant (Yehuda) */
  rgw_env.set("HTTP_HOST", "");

  /* XXX and -then- bloat up req_state with string copies from it */
  struct req_state rstate(req->cct, &rgw_env, req->id);
  struct req_state *s = &rstate;

  // XXX fix this
  s->cio = &io;

  RGWObjectCtx rados_ctx(store, s); // XXX holds map

  auto sysobj_ctx = store->svc()->sysobj->init_obj_ctx();
  s->sysobj_ctx = &sysobj_ctx;

  /* XXX and -then- stash req_state pointers everywhere they are needed */
  ret = req->init(rgw_env, &rados_ctx, &io, s);
  if (ret < 0)
  {
      goto done;
  }

  /* req is-a RGWOp, currently initialized separately */
  ret = req->op_init();
  if (ret < 0)
  {
      goto done;
  }

  /* now expected by rgw_log_op() */
  rgw_env.set("REQUEST_METHOD", s->info.method);
  rgw_env.set("REQUEST_URI", s->info.request_uri);
  rgw_env.set("QUERY_STRING", "");

  try
  {
    /* XXX authorize does less here then in the REST path, e.g.,
    * the user's info is cached, but still incomplete */
    ret = req->authorize(op);
    if (ret < 0)
    {
      goto done;
    }

    /* FIXME: remove this after switching all handlers to the new
    * authentication infrastructure. */
    if (!s->auth.identity)
    {
      s->auth.identity = rgw::auth::transform_old_authinfo(s);
    }

    ret = req->read_permissions(op);
    if (ret < 0)
    {
      goto done;
    }

    ret = op->init_processing();
    if (ret < 0)
    {
      goto done;
    }

    ret = op->verify_op_mask();
    if (ret < 0)
    {
      goto done;
    }

    ret = op->verify_permission();
    if (ret < 0)
    {
      if ((!s->system_request) && (!s->auth.identity->is_admin_of(s->user->get_id())))
      {
        goto done;
      }
    }

    ret = op->verify_params();
    if (ret < 0)
    {
      goto done;
    }

    op->pre_exec();
    op->execute();
    op->complete();
  }
  catch (const ceph::crypto::DigestException &e)
  {
      
  }

done:
  try
  {
    io.complete_request();
  }
  catch (rgw::io::Exception &e)
  {
  }

  return (ret < 0 ? ret : s->err.ret);
}