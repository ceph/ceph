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

#ifndef CEPH_RGW_CORS_S3_H
#define CEPH_RGW_CORS_S3_H

#include <map>
#include <string>
#include <iosfwd>
#include <expat.h>

#include <include/types.h>
#include <common/Formatter.h>
#include "rgw_xml.h"
#include "rgw_cors.h"

using namespace std;

class RGWCORSRule_S3 : public RGWCORSRule, public XMLObj
{
  public:
    RGWCORSRule_S3() {}
    ~RGWCORSRule_S3() {}
    
    bool xml_end(const char *el);
    void to_xml(XMLFormatter& f);
};

class RGWCORSConfiguration_S3 : public RGWCORSConfiguration, public XMLObj
{
  public:
    RGWCORSConfiguration_S3() {}
    ~RGWCORSConfiguration_S3() {}

    bool xml_end(const char *el);
    void to_xml(ostream& out);
};

class RGWCORSXMLParser_S3 : public RGWXMLParser
{
  CephContext *cct;

  XMLObj *alloc_obj(const char *el);
public:
  explicit RGWCORSXMLParser_S3(CephContext *_cct) : cct(_cct) {}
};
#endif /*CEPH_RGW_CORS_S3_H*/
