// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"

#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_loadgen.h"
#include "rgw_client_io.h"
#include "rgw_signal.h"

#include <atomic>

#define dout_subsys ceph_subsys_rgw

using namespace std;

void RGWLoadGenProcess::checkpoint()
{
  m_tp.drain(&req_wq);
}

void RGWLoadGenProcess::run()
{
  m_tp.start(); /* start thread pool */

  int i;

  int num_objs;

  conf->get_val("num_objs", 1000, &num_objs);

  int num_buckets;
  conf->get_val("num_buckets", 1, &num_buckets);

  vector<string> buckets(num_buckets);

  std::atomic<bool> failed = { false };

  for (i = 0; i < num_buckets; i++) {
    buckets[i] = "/loadgen";
    string& bucket = buckets[i];
    append_rand_alpha(cct, bucket, bucket, 16);

    /* first create a bucket */
    gen_request("PUT", bucket, 0, &failed);
    checkpoint();
  }

  string *objs = new string[num_objs];

  if (failed) {
    derr << "ERROR: bucket creation failed" << dendl;
    goto done;
  }

  for (i = 0; i < num_objs; i++) {
    char buf[16 + 1];
    gen_rand_alphanumeric(cct, buf, sizeof(buf));
    buf[16] = '\0';
    objs[i] = buckets[i % num_buckets] + "/" + buf;
  }

  for (i = 0; i < num_objs; i++) {
    gen_request("PUT", objs[i], 4096, &failed);
  }

  checkpoint();

  if (failed) {
    derr << "ERROR: bucket creation failed" << dendl;
    goto done;
  }

  for (i = 0; i < num_objs; i++) {
    gen_request("GET", objs[i], 4096, NULL);
  }

  checkpoint();

  for (i = 0; i < num_objs; i++) {
    gen_request("DELETE", objs[i], 0, NULL);
  }

  checkpoint();

  for (i = 0; i < num_buckets; i++) {
    gen_request("DELETE", buckets[i], 0, NULL);
  }

done:
  checkpoint();

  m_tp.stop();

  delete[] objs;

  rgw::signal::signal_shutdown();
} /* RGWLoadGenProcess::run() */

void RGWLoadGenProcess::gen_request(const string& method,
				    const string& resource,
				    int content_length, std::atomic<bool>* fail_flag)
{
  RGWLoadGenRequest* req =
    new RGWLoadGenRequest(env.driver->get_new_req_id(), method, resource,
			  content_length, fail_flag);
  dout(10) << "allocated request req=" << hex << req << dec << dendl;
  req_throttle.get(1);
  req_wq.queue(req);
} /* RGWLoadGenProcess::gen_request */

void RGWLoadGenProcess::handle_request(const DoutPrefixProvider *dpp, RGWRequest* r)
{
  RGWLoadGenRequest* req = static_cast<RGWLoadGenRequest*>(r);

  RGWLoadGenRequestEnv renv;

  utime_t tm = ceph_clock_now();

  renv.port = 80;
  renv.content_length = req->content_length;
  renv.content_type = "binary/octet-stream";
  renv.request_method = req->method;
  renv.uri = req->resource;
  renv.set_date(tm);
  renv.sign(dpp, access_key);

  RGWLoadGenIO real_client_io(&renv);
  RGWRestfulIO client_io(cct, &real_client_io);
  int ret = process_request(env, req, uri_prefix, &client_io,
                            null_yield, nullptr, nullptr, nullptr);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;

    if (req->fail_flag) {
      req->fail_flag++;
    }
  }

  delete req;
} /* RGWLoadGenProcess::handle_request */
