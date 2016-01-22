// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <sys/types.h>
#include <string.h>

#include "include/types.h"
#include "include/rados/librgw.h"
#include "rgw/rgw_acl_s3.h"
#include "rgw_acl.h"

#include "include/str_list.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include "rgw_rados.h"
#include "rgw_resolve.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_rest_user.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_lib.h"
#include "rgw_auth_s3.h"
#include "rgw_lib.h"

#include <errno.h>
#include <sstream>
#include <string.h>

#define dout_subsys ceph_subsys_rgw

int librgw_create(librgw_t* rgw, const char* const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }
  CephContext* cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->set_val("log_to_stderr", "false"); // quiet by default
  cct->_conf->set_val("err_to_stderr", "true"); // quiet by default
  cct->_conf->parse_env(); // environment variables override
  cct->_conf->apply_changes(NULL);

  common_init_finish(cct);
  *rgw = cct;
  return 0;
}

int librgw_acl_bin2xml(librgw_t rgw, const char* bin, int bin_len, char** xml)
{
  CephContext* cct = static_cast<CephContext*>(rgw);
  try {
    // convert to bufferlist
    bufferlist bl;
    bl.append(bin, bin_len);

    // convert to RGWAccessControlPolicy
    RGWAccessControlPolicy_S3 acl(cct);
    bufferlist::iterator bli(bl.begin());
    acl.decode(bli);

    // convert to XML stringstream
    stringstream ss;
    acl.to_xml(ss);

    // convert to XML C string
    *xml = strdup(ss.str().c_str());
    if (!*xml)
      return -ENOBUFS;
    return 0;
  }
  catch (const std::exception& e) {
    lderr(cct) << "librgw_acl_bin2xml: caught exception " << e.what() << dendl;
    return -2000;
  }
  catch (...) {
    lderr(cct) << "librgw_acl_bin2xml: caught unknown exception " << dendl;
    return -2000;
  }
}

void librgw_free_xml(librgw_t rgw, char *xml)
{
  free(xml);
}

int librgw_acl_xml2bin(librgw_t rgw, const char* xml, char** bin, int* bin_len)
{
  CephContext* cct = static_cast<CephContext*>(rgw);
  char *bin_ = NULL;
  try {
    RGWACLXMLParser_S3 parser(cct);
    if (!parser.init()) {
      return -1000;
    }
    if (!parser.parse(xml, strlen(xml), true)) {
      return -EINVAL;
    }
    RGWAccessControlPolicy_S3* policy =
      (RGWAccessControlPolicy_S3*)parser.find_first("AccessControlPolicy");
    if (!policy) {
      return -1001;
    }
    bufferlist bl;
    policy->encode(bl);

    bin_ = (char*)malloc(bl.length());
    if (!bin_) {
      return -ENOBUFS;
    }
    int bin_len_ = bl.length();
    bl.copy(0, bin_len_, bin_);

    *bin = bin_;
    *bin_len = bin_len_;
    return 0;
  }
  catch (const std::exception& e) {
    lderr(cct) << "librgw_acl_bin2xml: caught exception " << e.what() << dendl;
  }
  catch (...) {
    lderr(cct) << "librgw_acl_bin2xml: caught unknown exception " << dendl;
  }
  if (!bin_)
    free(bin_);
  bin_ = NULL;
  return -2000;
}

void librgw_free_bin(librgw_t rgw, char* bin)
{
  free(bin);
}

void librgw_shutdown(librgw_t rgw)
{
  CephContext* cct = static_cast<CephContext*>(rgw);
  cct->put();
}

class C_InitTimeout : public Context {
public:
  C_InitTimeout() {}
  void finish(int r) {
    derr << "Initialization timeout, failed to initialize" << dendl;
    exit(1);
  }
};

struct RGWLibRequest : public RGWRequest {
  string method;
  string resource;
  int content_length;
  atomic_t* fail_flag;

  RGWLibRequest(uint64_t req_id, const string& _m, const  string& _r, int _cl,
		bool user_command, atomic_t* _ff)
    :  RGWRequest(req_id), method(_m), resource(_r), content_length(_cl),
       fail_flag(_ff)
    {
      s->librgw_user_command =  user_command;
    }
};

void RGWLibRequestEnv::set_date(utime_t& tm)
{
  stringstream s;
  tm.asctime(s);
  date_str = s.str();
}

int RGWLibRequestEnv::sign(RGWAccessKey& access_key)
{
  map<string, string> meta_map;
  map<string, string> sub_resources;

  string canonical_header;
  string digest;

  rgw_create_s3_canonical_header(request_method.c_str(),
				 NULL, /* const char* content_md5 */
				 content_type.c_str(),
				 date_str.c_str(),
				 meta_map,
				 uri.c_str(),
				 sub_resources,
				 canonical_header);

  int ret = rgw_get_s3_header_digest(canonical_header, access_key.key, digest);
  if (ret < 0) {
    return ret;
  }
  return 0;
};

class RGWLibFrontend : public RGWProcessFrontend {
public:
  RGWLibFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf)
    : RGWProcessFrontend(pe, _conf) {}
  int init();
  void gen_request(const string& method, const string& resource,
		   int content_length, bool user_command,
		   atomic_t* fail_flag);
};

class RGWLibProcess : public RGWProcess {
    RGWAccessKey access_key;
public:
  RGWLibProcess(CephContext* cct, RGWProcessEnv* pe, int num_threads,
		RGWFrontendConfig* _conf) :
    RGWProcess(cct, pe, num_threads, _conf) {}
  void run();
  void checkpoint();
  void handle_request(RGWRequest* req);
  void gen_request(const string& method, const string& resource,
		   int content_length, bool user_command,
		   atomic_t* fail_flag);
  void set_access_key(RGWAccessKey& key) { access_key = key; }
};

void RGWLibProcess::checkpoint()
{
    m_tp.drain(&req_wq);
}

void RGWLibProcess::run()
{
  /* XXX */
}

void RGWLibProcess::gen_request(const string& method, const string& resource,
				int content_length, bool user_command,
				atomic_t* fail_flag)
{
  RGWLibRequest* req = new RGWLibRequest(store->get_new_req_id(), method,
					 resource, content_length,
					 user_command, fail_flag);
  dout(10) << "allocated request req=" << hex << req << dec << dendl;
  req_throttle.get(1);
  req_wq.queue(req);
}

void RGWLibProcess::handle_request(RGWRequest* r)
{
  RGWLibRequest* req = static_cast<RGWLibRequest*>(r);

  RGWLibRequestEnv env;

  utime_t tm = ceph_clock_now(NULL);

  env.port = 80;
  env.content_length = req->content_length;
  env.content_type = "binary/octet-stream"; /* TBD */
  env.request_method = req->method;
  env.uri = req->resource;
  env.set_date(tm);
  env.sign(access_key);

  RGWLibIO client_io(&env);

  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;

  }
  delete req;
}

void RGWLibFrontend::gen_request(const string& method, const string& resource,
				 int content_length, bool user_command,
				 atomic_t* fail_flag)
{
  RGWLibProcess* lib_process = static_cast<RGWLibProcess*>(pprocess);
  lib_process->gen_request(method, resource, content_length, user_command,
			   fail_flag);
}

int RGWLib::init()
{
  int r = 0;
  /* alternative default for module */
  vector<const char *> def_args;
  def_args.push_back("--debug-rgw=1/5");
  def_args.push_back("--keyring=$rgw_data/keyring");
  def_args.push_back("--log-file=/var/log/radosgw/$cluster-$name.log");

  vector<const char*> args;
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT,
	      CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf->rgw_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  common_init_finish(g_ceph_context);

  rgw_tools_init(g_ceph_context);

  rgw_init_resolver();

  store = RGWStoreManager::get_storage(g_ceph_context,
      g_conf->rgw_enable_gc_threads, g_conf->rgw_enable_quota_threads);

  if (!store) {
    mutex.Lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.Unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }

  r = rgw_perf_start(g_ceph_context);

  rgw_rest_init(g_ceph_context, store->region);

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  if (r)
    return 1;

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);
  rgw_log_usage_init(g_ceph_context, store);

  mgr = new RGWRESTMgr_Lib();
  mgr->set_logging(true);
  rest.register_default_mgr(mgr);

  if (!g_conf->rgw_ops_log_socket_path.empty()) {
    olog = new OpsLogSocket(g_ceph_context, g_conf->rgw_ops_log_data_backlog);
    olog->init(g_conf->rgw_ops_log_socket_path);
  }

  int port = 80;
  RGWProcessEnv env = { store, &rest, olog, port };

  fec = new RGWFrontendConfig("librgw");
  fe = new RGWLibFrontend(env, fec);

  fe->init();
  if (r < 0) {
    derr << "ERROR: failed initializing frontend" << dendl;
    return -r;
  }

  fe->run();

  return 0;

}

int RGWLib::stop()
{
  derr << "shutting down" << dendl;

  fe->stop();

  fe->join();

  delete fe;

  rgw_log_usage_finalize();

  delete olog;

  RGWStoreManager::close_storage(store);

  rgw_tools_cleanup();
  rgw_shutdown_resolver();

  rgw_perf_stop(g_ceph_context);

  dout(1) << "final shutdown" << dendl;
  g_ceph_context->put();

  ceph::crypto::shutdown();

  return 0;
}

int RGWLib::get_uri(const uint64_t handle, string& uri)
{
  ceph::unordered_map<uint64_t, string>::iterator i = handles_map.find(handle);
  if (i != handles_map.end()) {
    uri =  i->second;
    return 0;
  }
  return -1;
}

uint64_t RGWLib::get_new_handle(const string& uri)
{
  ceph::unordered_map<string, uint64_t>::iterator i =
    allocated_objects_handles.find(uri);
  if (i != allocated_objects_handles.end()) {
    return i->second;
  }

  allocated_objects_handles[uri] = last_allocated_handle.inc();
  handles_map[last_allocated_handle.read()] = uri;

  return last_allocated_handle.read();
}

int RGWLibIO::set_uid(RGWRados *store, const rgw_user& uid)
{
  int ret = rgw_get_user_info_by_uid(store, uid, user_info, NULL);
  if (ret < 0) {
    derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	 << ret << dendl;
  }
  return ret;
}

int RGWLibIO::write_data(const char* buf, int len)
{
  return len;
}

int RGWLibIO::read_data(char* buf, int len)
{
  int read_len = MIN(left_to_read, (uint64_t)len);
  left_to_read -= read_len;
  return read_len;
}

void RGWLibIO::flush()
{
  /* XXX */
}

int RGWLibIO::complete_request()
{
  return 0;
}

void RGWLibIO::init_env(CephContext* cct)
{
  env.init(cct);

  left_to_read = re->content_length;

  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)re->content_length);
  env.set("CONTENT_LENGTH", buf);

  env.set("CONTENT_TYPE", re->content_type.c_str());
  env.set("HTTP_DATE", re->date_str.c_str());

  for (map<string, string>::iterator iter = re->headers.begin();
       iter != re->headers.end(); ++iter) {
    env.set(iter->first.c_str(), iter->second.c_str());
  }

  env.set("REQUEST_METHOD", re->request_method.c_str());
  env.set("REQUEST_URI", re->uri.c_str());
  env.set("QUERY_STRING", re->query_string.c_str());
  env.set("SCRIPT_URI", re->uri.c_str());

  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%d", re->port);
  env.set("SERVER_PORT", port_buf);
}

int RGWLibIO::send_status(int status, const char *status_name)
{
  return 0;
}

int RGWLibIO::send_100_continue()
{
  return 0;
}

int RGWLibIO::complete_header()
{
  return 0;
}

int RGWLibIO::send_content_length(uint64_t len)
{
  return 0;
}

int RGWLib::get_userinfo_by_uid(const string& uid, RGWUserInfo& info)
{
  atomic_t failed;
  fe->gen_request("GET", uid, 4096, true, &failed);
  return failed.read();
}

int RGWLib::get_user_acl()
{
  return 0;
}

int RGWLib::set_user_permissions()
{
  return 0;
}

int RGWLib::set_user_quota()
{
  return 0;
}

int RGWLib::get_user_quota()
{
  return 0;
}

int RGWLib::get_user_buckets_list()
{
  /* need to authanitcate first */
  atomic_t failed;
  fe->gen_request("GET", "", 4096, false, &failed);
  return failed.read();

}

int RGWLib::get_bucket_objects_list()
{
  return 0;
}

int RGWLib::create_bucket()
{
  return 0;
}

int RGWLib::delete_bucket()
{
  return 0;
}

int RGWLib::get_bucket_attributes()
{
  return 0;
}

int RGWLib::set_bucket_attributes()
{
  return 0;
}

int RGWLib::create_object()
{
  return 0;
}

int RGWLib::delete_object()
{
  return 0;
}

int RGWLib::write()
{
  return 0;
}

int RGWLib::read()
{
  return 0;
}

int RGWLib::get_object_attributes()
{
  return 0;
}

int RGWLib::set_object_attributes()
{
  return 0;
}

/* global RGW library object */
static RGWLib rgwlib;

int librgw_init()
{
  return rgwlib.init();
}

int librgw_stop()
{
  return rgwlib.stop();
}
