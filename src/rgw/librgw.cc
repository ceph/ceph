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
#include "rgw/rgw_acl.h"
#include "rgw_acl.h"

#include <errno.h>
#include <sstream>
#include <string.h>

int librgw_acl_bin2xml(const char *bin, int bin_len, char **xml)
{
  try {
    // convert to bufferlist
    bufferlist bl;
    bl.append(bin, bin_len);

    // convert to RGWAccessControlPolicy
    RGWAccessControlPolicy acl;
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
  catch (...) {
    return -2000;
  }
}

void librgw_free_xml(char *xml)
{
  free(xml);
}

int librgw_acl_xml2bin(const char *xml, char **bin, int *bin_len)
{
  char *bin_ = NULL;
  try {
    RGWXMLParser parser;
    if (!parser.init()) {
      return -1000;
    }
    if (!parser.parse(xml, strlen(xml), true)) {
      return -EINVAL;
    }
    RGWAccessControlPolicy *policy =
      (RGWAccessControlPolicy *)parser.find_first("AccessControlPolicy");
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
  catch (...) {
    if (!bin_)
      free(bin_);
    bin_ = NULL;
    return -2000;
  }
}

void librgw_free_bin(char *bin)
{
  free(bin);
}
