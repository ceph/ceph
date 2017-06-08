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

#include "include/types.h"
#include "include/rados/librgw.h"
#include "rgw/rgw_acl_s3.h"
#include "rgw_acl.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include <errno.h>
#include <sstream>
#include <string.h>

#define dout_subsys ceph_subsys_rgw

int librgw_create(librgw_t *rgw, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }
  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->set_val("log_to_stderr", "false"); // quiet by default
  cct->_conf->set_val("err_to_stderr", "true"); // quiet by default
  cct->_conf->parse_env(); // environment variables override
  cct->_conf->apply_changes(NULL);

  common_init_finish(cct);
  *rgw = cct;
  return 0;
}

int librgw_acl_bin2xml(librgw_t rgw, const char *bin, int bin_len, char **xml)
{
  try {
    // convert to bufferlist
    bufferlist bl;
    bl.append(bin, bin_len);

    // convert to RGWAccessControlPolicy
    RGWAccessControlPolicy_S3 acl((CephContext *)rgw);
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
  catch (const std::exception &e) {
    lderr(rgw) << "librgw_acl_bin2xml: caught exception " << e.what() << dendl;
    return -2000;
  }
  catch (...) {
    lderr(rgw) << "librgw_acl_bin2xml: caught unknown exception " << dendl;
    return -2000;
  }
}

void librgw_free_xml(librgw_t rgw, char *xml)
{
  free(xml);
}

int librgw_acl_xml2bin(librgw_t rgw, const char *xml, char **bin, int *bin_len)
{
  char *bin_ = NULL;
  try {
    RGWACLXMLParser_S3 parser((CephContext *)rgw);
    if (!parser.init()) {
      return -1000;
    }
    if (!parser.parse(xml, strlen(xml), true)) {
      return -EINVAL;
    }
    RGWAccessControlPolicy_S3 *policy =
      (RGWAccessControlPolicy_S3 *)parser.find_first("AccessControlPolicy");
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
  catch (const std::exception &e) {
    lderr(rgw) << "librgw_acl_bin2xml: caught exception " << e.what() << dendl;
  }
  catch (...) {
    lderr(rgw) << "librgw_acl_bin2xml: caught unknown exception " << dendl;
  }
  if (!bin_)
    free(bin_);
  bin_ = NULL;
  return -2000;
}

void librgw_free_bin(librgw_t rgw, char *bin)
{
  free(bin);
}

void librgw_shutdown(librgw_t rgw)
{
  rgw->put();
}
