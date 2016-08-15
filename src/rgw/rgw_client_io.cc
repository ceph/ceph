// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "rgw_client_io.h"

#define dout_subsys ceph_subsys_rgw

void RGWClientIO::init(CephContext *cct) {
  init_env(cct);

  if (cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    std::map<string, string, ltstr_nocase>& env_map = get_env().get_map();
    std::map<string, string, ltstr_nocase>::iterator iter = env_map.begin();

    for (iter = env_map.begin(); iter != env_map.end(); ++iter) {
      ldout(cct, 20) << iter->first << "=" << iter->second << dendl;
    }
  }
}


int RGWStreamIO::recv_body(char *buf, std::size_t max, bool calculate_hash)
{
  try {
    const auto sent = recv_body(buf, max);

    if (calculate_hash) {
      if (! sha256_hash) {
        sha256_hash = calc_hash_sha256_open_stream();
      }
      calc_hash_sha256_update_stream(sha256_hash, buf, sent);
    }
    return sent;
  } catch (RGWStreamIOEngine::Exception& e) {
    return e.value();
  }
}

string RGWStreamIO::grab_aws4_sha256_hash()
{
  return calc_hash_sha256_close_stream(&sha256_hash);
}
